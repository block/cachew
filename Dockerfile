ARG ALPINE_VERSION=3.21
FROM alpine:${ALPINE_VERSION}

ARG TARGETARCH
ARG TARGETPLATFORM

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

# Install runtime dependencies for git operations and TLS
RUN apk add --no-cache ca-certificates git

# Set working directory (config uses relative paths like ./state/cache)
WORKDIR /app

# Copy pre-built binary for the target architecture
COPY dist/cachewd-linux-${TARGETARCH} /usr/local/bin/cachewd

# Copy default configuration file
COPY cachew.hcl /app/cachew.hcl

# Bind to all interfaces in Docker (can be overridden with CACHEW_BIND env var)
ENV CACHEW_BIND=0.0.0.0:8080

ENTRYPOINT ["/usr/local/bin/cachewd"]
CMD ["--config", "/app/cachew.hcl"]
