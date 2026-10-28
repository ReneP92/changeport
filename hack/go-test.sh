#!/usr/bin/env bash
# Runs the Go tests. The controller's tests run against a real API server
# (envtest) with the CRDs of Changeport, Strimzi and cert-manager, so this
# fetches the pinned upstream files and envtest's binaries first. The Postgres
# CDC source's checks run against Postgres servers in Docker
# (internal/cdc/postgres_test.go).
# Usage: hack/go-test.sh [go test flags]
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

fetch_upstream_files
KUBEBUILDER_ASSETS=$(envtest_assets)
export KUBEBUILDER_ASSETS
docker info >/dev/null 2>&1 || die "Docker is not running; the Postgres CDC source's tests run Postgres in it"
export CHANGEPORT_POSTGRES_TESTS=1
go test -race "$@" ./...
