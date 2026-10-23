# syntax=docker/dockerfile:1

# The Changeport console (cmd/console) with the web app (web/) built in.
# Build it from the repository root with make console-image.

# Bun, pinned by the digest of its index, installs and builds the web app on
# the build machine's own platform; the output is plain static files.
FROM --platform=$BUILDPLATFORM oven/bun:1.4.3@sha256:ec06c3b6cea04192ae6770c434f668ca41d343ad19fa6472216c7b48be39c598 AS web
WORKDIR /src/web
COPY web/package.json web/bun.lock web/bunfig.toml ./
RUN --mount=type=cache,target=/root/.bun/install/cache bun install --frozen-lockfile
COPY web/ ./
RUN bun run build

# The Go toolchain matching go.mod, pinned by the digest of its index. The
# binary is cross-compiled on the build machine's own platform, with the web
# app embedded (-tags embedui).
FROM --platform=$BUILDPLATFORM golang:1.26.5@sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
COPY --from=web /src/web/dist internal/console/ui/dist
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -tags embedui -trimpath -ldflags=-s -o /console ./cmd/console

# A static binary on a minimal, non-root base: no shell, no package manager.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
LABEL org.opencontainers.image.title="Changeport console" \
      org.opencontainers.image.source="https://github.com/ReneP92/changeport"
COPY --from=build /console /console
ENTRYPOINT ["/console"]
