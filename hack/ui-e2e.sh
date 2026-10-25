#!/usr/bin/env bash
# Runs the console's Playwright tests against the console on kind
# (http://localhost:8088), signed in as Clerk test users: the setup creates
# this run's users, organizations and customers in the Clerk development
# instance of .env, and the teardown removes them. Run just dev-up first, with
# .env filled in (see .env.sample).
# Usage: hack/ui-e2e.sh [playwright arguments]
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ensure_kind
ensure_kubectl
cluster_exists || die "kind cluster $CLUSTER_NAME does not exist; run just dev-up first"
config=$(console_config_status)
[[ $config == 200 ]] ||
  die "the console at http://localhost:$CONSOLE_PORT is not configured (GET /api/config answered $config); fill in .env and run just console-config"

publishable_key=$(dotenv_value CLERK_PUBLISHABLE_KEY)
[[ -n $publishable_key ]] || publishable_key=$(dotenv_value VITE_CLERK_PUBLISHABLE_KEY)
CLERK_PUBLISHABLE_KEY=$publishable_key
CLERK_SECRET_KEY=$(dotenv_value CLERK_SECRET_KEY)
CHANGEPORT_STAFF_ORG_ID=$(dotenv_value CHANGEPORT_STAFF_ORG_ID)
[[ -n $CLERK_SECRET_KEY && -n $CHANGEPORT_STAFF_ORG_ID ]] || die ".env lacks the Clerk secret key or the staff organization ID"
export CLERK_PUBLISHABLE_KEY CLERK_SECRET_KEY CHANGEPORT_STAFF_ORG_ID
# The tests create and delete customers with the pinned kubectl.
export KUBECTL=$KUBECTL_BIN KUBE_CONTEXT
export PLAYWRIGHT_BASE_URL="http://localhost:$CONSOLE_PORT"

hack/bun.sh install --frozen-lockfile >/dev/null
hack/bun.sh x playwright install ${CI:+--with-deps} chromium >/dev/null
hack/bun.sh run e2e "$@"
