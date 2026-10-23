#!/usr/bin/env bash
# Tests the console on kind the way a browser reaches it, at
# http://localhost:8088: its health, the web app with its Content Security
# Policy, an API that refuses requests without a valid session, and, in
# Chromium, that an unconfigured console says so and a configured one shows
# Clerk's sign-in without Content Security Policy violations.
# Usage: hack/console-test.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

url="http://localhost:$CONSOLE_PORT"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Prints the HTTP status of a GET, saving the body and headers.
status() { curl -s -o "$work/body" -D "$work/headers" -w '%{http_code}' "${@:2}" "$url$1"; }

[[ $(status /healthz) == 200 ]] || die "the console at $url is not healthy"
if [[ $(status /) != 200 ]] || ! grep -q '<div id="root">' "$work/body"; then
  die "the console at $url does not serve the web app:"$'\n'"$(head -c 300 "$work/body")"
fi
grep -qi '^content-security-policy:.*frame-ancestors' "$work/headers" || die "the web app has no Content Security Policy"

case $(status /api/config) in
  200) configured=true ;;
  503) configured=false ;;
  *) die "GET /api/config answered $(cat "$work/body")" ;;
esac
refused=$([[ $configured == true ]] && echo 401 || echo 503)
for token in "" "not-a-token"; do
  code=$(status /api/me ${token:+-H "Authorization: Bearer $token"})
  [[ $code == "$refused" ]] || die "GET /api/me with token '$token' answered $code, want $refused"
done
log "The console's API refuses requests without a valid session ($refused; configured: $configured)"

log "Opening the console in Chromium"
hack/bun.sh install --frozen-lockfile >/dev/null
hack/bun.sh x playwright install ${CI:+--with-deps} chromium >/dev/null
PLAYWRIGHT_BASE_URL=$url hack/bun.sh run e2e
log "Console test passed"
