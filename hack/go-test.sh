#!/usr/bin/env bash
# Runs the Go tests. The controller's tests run against a real API server
# (envtest) with the CRDs of Changeport, Strimzi and cert-manager, so this
# fetches the pinned upstream files and envtest's binaries first.
# Usage: hack/go-test.sh [go test flags]
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

fetch_upstream_files
KUBEBUILDER_ASSETS=$(envtest_assets)
export KUBEBUILDER_ASSETS
go test -race "$@" ./...
