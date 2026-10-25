# syntax=docker/dockerfile:1

# The Changeport Kafka agent (cmd/agent), which runs next to every customer's
# Kafka cluster. Build it from the repository root with just agent-image.

# The Go toolchain matching go.mod, pinned by the digest of its index. The
# binary is cross-compiled on the build machine's own platform.
FROM --platform=$BUILDPLATFORM golang:1.26.5@sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags=-s -o /agent ./cmd/agent

# A static binary on a minimal, non-root base: no shell, no package manager.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
LABEL org.opencontainers.image.title="Changeport agent" \
      org.opencontainers.image.source="https://github.com/ReneP92/changeport"
COPY --from=build /agent /agent
ENTRYPOINT ["/agent"]
