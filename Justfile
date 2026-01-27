set positional-arguments := true
set shell := ["bash", "-c"]

# Configuration

VERSION := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
GIT_COMMIT := `git rev-parse HEAD 2>/dev/null || echo "unknown"`
TAG := `git rev-parse --short HEAD 2>/dev/null || echo "dev"`
RELEASE := "dist"
REGISTRY := env_var_or_default("REGISTRY", "268271485387.dkr.ecr.us-west-2.amazonaws.com")
IMAGE_NAME := "cachew"
IMAGE_REF := REGISTRY + "/" + IMAGE_NAME + ":" + TAG
IMAGE_LATEST := REGISTRY + "/" + IMAGE_NAME + ":latest"

_help:
    @just -l

# Run tests
test:
    @gotestsum --hide-summary output,skipped --format-hide-empty-pkg ${CI:+--format github-actions} ./... -- -race -timeout 30s

# Lint code
lint:
    golangci-lint run
    actionlint

# Format code
fmt:
    just --unstable --fmt
    golangci-lint fmt
    go mod tidy

# ============================================================================
# Build
# ============================================================================

# Build for current platform
build:
    @mkdir -p {{ RELEASE }}
    @go build -trimpath -o {{ RELEASE }}/cachewd \
        -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" \
        ./cmd/cachewd
    @echo "✓ Built {{ RELEASE }}/cachewd"

# Build for Linux (current arch)
build-linux:
    #!/usr/bin/env bash
    set -e
    mkdir -p {{ RELEASE }}
    ARCH=$(uname -m)
    [[ "$ARCH" == "x86_64" ]] && ARCH="amd64"
    [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]] && ARCH="arm64"
    echo "Building for linux/${ARCH}..."
    GOOS=linux GOARCH=${ARCH} go build -trimpath \
        -o {{ RELEASE }}/cachewd-linux-${ARCH} \
        -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" \
        ./cmd/cachewd
    echo "✓ Built {{ RELEASE }}/cachewd-linux-${ARCH}"

# Build all platforms
build-all:
    @mkdir -p {{ RELEASE }}
    @echo "Building all platforms..."
    @GOOS=darwin GOARCH=arm64 go build -trimpath -o {{ RELEASE }}/cachewd-darwin-arm64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @GOOS=darwin GOARCH=amd64 go build -trimpath -o {{ RELEASE }}/cachewd-darwin-amd64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @GOOS=linux GOARCH=arm64 go build -trimpath -o {{ RELEASE }}/cachewd-linux-arm64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @GOOS=linux GOARCH=amd64 go build -trimpath -o {{ RELEASE }}/cachewd-linux-amd64 -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" ./cmd/cachewd
    @echo "✓ Built all platforms"

# ============================================================================
# Docker
# ============================================================================

# Build single-arch Docker image
docker-build: build-linux
    #!/usr/bin/env bash
    set -e
    ARCH=$(uname -m)
    [[ "$ARCH" == "x86_64" ]] && ARCH="amd64"
    [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]] && ARCH="arm64"
    echo "Building Docker image for linux/${ARCH}..."
    docker build --platform linux/${ARCH} -t {{ IMAGE_NAME }}:local .
    echo "✓ Built {{ IMAGE_NAME }}:local"

# Build multi-arch Docker image
docker-build-multi: build-all
    #!/usr/bin/env bash
    set -e
    BUILDER="multi-arch-{{ IMAGE_NAME }}"
    docker buildx create --driver docker-container --driver-opt image=moby/buildkit:v0.17.3 --name "$BUILDER" 2>/dev/null || true
    docker buildx use "$BUILDER"
    echo "Building multi-arch image..."
    docker buildx build --platform linux/amd64,linux/arm64 \
        -t {{ IMAGE_REF }} -t {{ IMAGE_LATEST }} .
    echo "✓ Built multi-arch image"

# ============================================================================
# Run
# ============================================================================

# Run natively
run: build
    @echo "→ Starting cachew at http://localhost:8080"
    @mkdir -p state
    @{{ RELEASE }}/cachewd --config cachew.hcl

# Run in Docker (optionally with debug: just docker-run debug)
docker-run log_level="info": docker-build
    @echo "→ Starting cachew at http://localhost:8080 (log-level={{ log_level }})"
    @docker run --rm -it -p 8080:8080 -v $(pwd)/state:/app/state --name {{ IMAGE_NAME }} {{ IMAGE_NAME }}:local --log-level={{ log_level }}

# Clean up build artifacts and Docker images
docker-clean:
    @echo "Cleaning..."
    @rm -rf {{ RELEASE }}
    @docker rmi {{ IMAGE_NAME }}:local 2>/dev/null || true
    @docker rmi {{ IMAGE_REF }} 2>/dev/null || true
    @docker rmi {{ IMAGE_LATEST }} 2>/dev/null || true
    @echo "✓ Cleaned"

# ============================================================================
# ECR Push
# ============================================================================

# Push to ECR
image-push tag="latest": build-all
    #!/usr/bin/env bash
    set -e

    # Login to ECR
    echo "→ Authenticating to ECR..."
    aws-creds sync
    export AWS_REGION="us-west-2"
    export AWS_PROFILE=block-coder-comp-staging--dx-admin-operator

    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"
    aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY
    aws ecr create-repository --repository-name {{ IMAGE_NAME }} --region $AWS_REGION 2>/dev/null || true

    # Build and push
    BUILDER="multi-arch-{{ IMAGE_NAME }}"
    docker buildx rm "$BUILDER" 2>/dev/null || true
    docker buildx create --driver docker-container --driver-opt image=moby/buildkit:v0.17.3 --name "$BUILDER"
    docker buildx use "$BUILDER"

    TAGS="-t {{ IMAGE_REF }}"
    [[ "{{ tag }}" == "latest" ]] && TAGS="$TAGS -t {{ IMAGE_LATEST }}"

    echo "→ Pushing multi-arch image..."
    docker buildx build --platform linux/amd64,linux/arm64 $TAGS --push .
    echo "✓ Pushed to ECR"

# Push to ECR (CI - no login)
image-push-ci tag="latest": build-all
    #!/usr/bin/env bash
    set -e
    BUILDER="multi-arch-{{ IMAGE_NAME }}"
    docker buildx rm "$BUILDER" 2>/dev/null || true
    docker buildx create --driver docker-container --driver-opt image=moby/buildkit:v0.17.3 --name "$BUILDER"
    docker buildx use "$BUILDER"

    TAGS="-t {{ IMAGE_REF }}"
    [[ "{{ tag }}" == "latest" ]] && TAGS="$TAGS -t {{ IMAGE_LATEST }}"

    echo "→ Pushing multi-arch image..."
    docker buildx build --platform linux/amd64,linux/arm64 $TAGS --push .
    echo "✓ Pushed to ECR"
