# Build stage — runs on native arch, cross-compiles for target (no QEMU)
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

ARG TARGETARCH
ARG TARGETOS=linux

WORKDIR /src

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s" -o /nvmfplugin ./cmd/nvmfplugin/

# Runtime stage
FROM debian:13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        e2fsprogs \
        xfsprogs \
        btrfs-progs \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /nvmfplugin /nvmfplugin

ENTRYPOINT ["/nvmfplugin"]
