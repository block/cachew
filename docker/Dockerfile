# Runtime image - binaries are pre-built on the host
FROM alpine:3.21

ARG TARGETARCH

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

# Install runtime dependencies for git operations and TLS
RUN apk add --no-cache ca-certificates git tzdata && \
  addgroup -g 1000 cachew && \
  adduser -D -u 1000 -G cachew cachew

# Set working directory (config uses relative paths like ./state/cache)
WORKDIR /app

# Copy pre-built binary from host (built by Justfile)
COPY dist/cachewd-linux-${TARGETARCH} /usr/local/bin/cachewd

# Copy default configuration file
COPY cachew.hcl /app/cachew.hcl

# Create state directory with proper permissions
RUN mkdir -p /app/state/cache && \
  chown -R cachew:cachew /app

# Switch to non-root user
USER cachew

# Bind to all interfaces in Docker (can be overridden with CACHEW_BIND env var)
ENV CACHEW_BIND=0.0.0.0:8080

ENTRYPOINT ["/usr/local/bin/cachewd"]
CMD ["--config", "/app/cachew.hcl"]
