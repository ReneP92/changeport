#!/usr/bin/env bash
# Runs the pinned Bun in web/, downloading it into bin/ first if needed, so
# no Bun, Node or other package manager has to be installed.
# Usage: hack/bun.sh <bun arguments>, such as hack/bun.sh run lint
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ensure_bun
cd web
exec "$BUN_BIN" "$@"
