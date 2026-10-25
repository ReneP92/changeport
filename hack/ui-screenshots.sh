#!/usr/bin/env bash
# Captures every screen of the console on kind, on a desktop and a phone,
# into docs/screenshots: a Playwright tour signed in as Clerk test users,
# which provisions its own customers through the operator console and
# removes them again. Run just dev-up first, with .env filled in.
# Usage: hack/ui-screenshots.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

export CHANGEPORT_SCREENSHOTS_DIR="$ROOT/docs/screenshots"
rm -rf "$CHANGEPORT_SCREENSHOTS_DIR"
hack/ui-e2e.sh
log "The screenshots are in docs/screenshots"
