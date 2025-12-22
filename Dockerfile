# ----------------------------------------------------------------------------
# Build stage
# ----------------------------------------------------------------------------
FROM golang:1.25.5-bookworm AS builder

WORKDIR /src

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        make \
        pkg-config \
        libudev-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY . .

# Build the plugin (assumes Makefile default target builds ./bin/nvmfplugin)
RUN make

# ----------------------------------------------------------------------------
# Runtime stage
# ----------------------------------------------------------------------------
FROM debian:13

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        e2fsprogs \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy the built binary from the builder stage
COPY --from=builder /src/bin/nvmfplugin /nvmfplugin

ENTRYPOINT ["/nvmfplugin"]