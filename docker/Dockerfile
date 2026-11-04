# Runtime image - binaries are pre-built on the host
FROM alpine:3.23

ARG TARGETARCH
ARG GIT_LFS_VERSION=3.8.1

SHELL ["/bin/sh", "-o", "pipefail", "-c"]

# Install runtime dependencies for git operations and TLS
RUN apk add --no-cache 'ca-certificates>=20261020-r0' curl git git-daemon tzdata zstd && \
    case "${TARGETARCH}" in \
      amd64) git_lfs_sha=e72cf723a4a660438862282e8aecf3885fc8123e88bfcad534cf6b941a354b75 ;; \
      arm64) git_lfs_sha=67895dd77b63329796bc5efdd47b57f218e1f7b98717ebaf936a8238dbcaec0f ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    curl -fsSLo /tmp/git-lfs.tar.gz \
      "https://github.com/git-lfs/git-lfs/releases/download/v${GIT_LFS_VERSION}/git-lfs-linux-${TARGETARCH}-v${GIT_LFS_VERSION}.tar.gz" && \
    echo "${git_lfs_sha}  /tmp/git-lfs.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/git-lfs.tar.gz -C /tmp "git-lfs-${GIT_LFS_VERSION}/git-lfs" && \
    mv "/tmp/git-lfs-${GIT_LFS_VERSION}/git-lfs" /usr/local/bin/git-lfs && \
    chmod 0755 /usr/local/bin/git-lfs && \
    addgroup -g 1000 cachew && \
    adduser -D -u 1000 -G cachew cachew

# Set working directory (config uses relative paths like ./state/cache)
WORKDIR /app

# Copy pre-built binary from host (built by Justfile)
COPY dist/cachewd-linux-${TARGETARCH} /usr/local/bin/cachewd
COPY dist/cachew-linux-${TARGETARCH} /usr/local/bin/cachew

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
