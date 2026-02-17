set positional-arguments := true
set shell := ["bash", "-c"]

# Import modules
mod docker

# Configuration

VERSION := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
GIT_COMMIT := `git rev-parse HEAD 2>/dev/null || echo "unknown"`
TAG := `git rev-parse --short HEAD 2>/dev/null || echo "dev"`
RELEASE := "dist"
GOARCH := env("GOARCH", `go env GOARCH`)
GOOS := env("GOOS", `go env GOOS`)

_help:
    @just -l

# Run tests
test:
    @gotestsum --hide-summary output,skipped --format-hide-empty-pkg ${CI:+--format=github-actions} ./... ${CI:+--tags=integration} -race -timeout 30s

# Lint code
lint:
    golangci-lint run
    actionlint

# Format code
fmt:
    just --unstable --fmt
    git ls-files | grep '\.go$' | xargs gosimports -local github.com/block -w
    git ls-files | grep '\.go$' | xargs dirname | uniq | sed 's,^,./,' | xargs go fmt
    go mod tidy

# ============================================================================
# Build
# ============================================================================

# Build for current platform
build GOOS=(GOOS) GOARCH=(GOARCH):
    #!/usr/bin/env bash
    mkdir -p {{ RELEASE }}
    CGO_ENABLED=0 GOOS={{ GOOS }} GOARCH={{ GOARCH }} \
        go build -trimpath -o {{ RELEASE }}/cachewd-{{ GOOS }}-{{ GOARCH }} \
        -ldflags "-s -w -X main.version={{ VERSION }} -X main.gitCommit={{ GIT_COMMIT }}" \
        ./cmd/cachewd
    test "{{ GOOS }}-{{ GOARCH }}" = "$(go env GOOS)-$(go env GOARCH)" && (cd {{ RELEASE }} && ln -sf cachewd-{{ GOOS }}-{{ GOARCH }} cachewd)
    echo "✓ Built {{ RELEASE }}/cachewd-{{ GOOS }}-{{ GOARCH }}"

# Build all platforms
build-all:
    @mkdir -p {{ RELEASE }}
    @echo "Building all platforms..."
    just build darwin arm64
    just build darwin amd64
    just build linux arm64
    just build linux amd64
    @echo "✓ Built all platforms"

# ============================================================================
# Run
# ============================================================================

# Run dev server with hot reload
dev:
    @proctor

# Clean up build artifacts
clean:
    @echo "Cleaning..."
    @rm -rf {{ RELEASE }}
    @echo "✓ Cleaned"
